from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("country").orderBy(F.col("stock_qty").desc(), "id")
w2 = Window.partitionBy("country", "brand").orderBy("cost", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("brand").orderBy(F.col("store_sale_revenue").desc_nulls_last())

valid = (
    df_products
    .filter(F.col("brand").isNotNull())
    .withColumn("price_int", F.col("price").cast("int"))
)

enriched = (
    valid
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "left")
    .filter(F.upper(F.col("p.supplier_name")).like("%ACI%"))
    .select("e.*", F.col("p.supplier_name"), F.col("p.country"), F.col("p.rating"))
)

scored = (
    enriched
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "price_int",
        "supplier_name",
        "country",
        "rating",
        F.row_number().over(w_grp).alias("row_seq"),
        F.avg("price").over(w2).alias("price_avg_5_prior"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.revenue").alias("store_sale_revenue"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

with_windows = (
    with_latest
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_name",
        "rating",
        "price_avg_5_prior",
        "store_sale_sale_date",
        F.avg("stock_qty").over(w_recent).alias("running_avg_stock_qty"),
        F.count("stock_qty").over(w_recent).alias("running_count_stock_qty"),
        F.min("stock_qty").over(w_recent).alias("running_min_stock_qty"),
        F.cume_dist().over(w5).alias("store_sale_revenue_cume_dist"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("month", "launch_date").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_rows"),
        F.count("*").alias("event_count"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_rows"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_count"),
    )
    .orderBy(F.col("num_brands").desc())
)
