from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy(F.date_trunc("MONTH", "store_sale_sale_date"), "category").orderBy(F.col("store_sale_sale_date").desc(), "id")
w3 = Window.partitionBy("supplier_id")
w4 = Window.partitionBy("store_sale_revenue_tier").orderBy(F.col("electronics_supplier_id_avg_cost").desc())

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("cost") >= 320))
    .withColumn("product_name_length", F.length("product_name"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("NONE")))
)

with_latest = (
    valid
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

scored = (
    with_latest
    .filter(F.col("brand") == "Acme")
    .select(
        "id",
        "product_name",
        "category",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "brand_filled",
        "store_sale_sale_date",
        "store_sale_revenue",
        F.row_number().over(w_hist).alias("row_seq"),
        F.avg("cost").over(w3).alias("supplier_id_avg_cost"),
    )
)

filtered = (
    scored
    .filter(F.array_contains("tags", "sale"))
    .select(
        "id",
        "product_name",
        "category",
        "stock_qty",
        "launch_date",
        "tags",
        "brand_filled",
        "store_sale_sale_date",
        "store_sale_revenue",
        "row_seq",
        "supplier_id_avg_cost",
        F.weekofyear("launch_date").alias("launch_date_weekofyear"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("store_sale_revenue") >= 4400, "LARGE").otherwise("SMALL").alias("store_sale_revenue_tier"),
        F.date_trunc("QUARTER", "launch_date").alias("quarter_start"),
    )
    .agg(
        F.countDistinct("category").alias("distinct_categories"),
        F.percentile_approx("launch_date_weekofyear", 0.5).alias("launch_date_weekofyear_p50"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("supplier_id_avg_cost")).otherwise(0)).alias("electronics_supplier_id_avg_cost"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
    )
    .withColumn("electronics_supplier_id_avg_cost_rank", F.rank().over(w4))
)
