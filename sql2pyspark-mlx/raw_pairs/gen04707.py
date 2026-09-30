from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").desc(), "id")

latest = (
    df_inventory
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

filtered = (
    latest
    .filter(F.col("last_restock") >= F.lit("2023-07-21").cast("date"))
    .select(
        "id",
        "product_id",
        "last_restock",
        "rn",
        (F.col("rn") + F.col("quantity")).alias("rn_plus_quantity"),
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.product_name"), F.col("p.category"))
)

enriched_inventory = (
    enriched
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .select(
        "a.*",
        F.col("d.brand"),
        F.col("d.category").alias("product_category"),
        F.col("d.stock_qty"),
    )
)

result = (
    enriched_inventory
    .withColumn("rn_plus_quantity_tier", F.when(F.col("rn_plus_quantity") >= 76, "LARGE").otherwise("SMALL"))
    .withColumn("quarter_start", F.date_trunc("quarter", "last_restock"))
    .withColumn("activity_month", F.date_trunc("month", "last_restock_month_end"))
    .cube("rn_plus_quantity_tier", "quarter_start", "activity_month")
    .agg(
        F.round(F.sum("rn_plus_quantity") / F.nullif(F.count(F.when(F.col("brand") == "Initech", 1)), F.lit(0)), 2).alias("rn_plus_quantity_per_initech"),
        F.percentile_approx("rn_plus_quantity", [0.25, 0.5, 0.75]).alias("rn_plus_quantity_quartiles"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("stock_qty")).otherwise(0)).alias("electronics_stock_qty"),
        F.countDistinct("product_category").alias("distinct_product_categories"),
        F.round(F.sum("rn_plus_quantity") / F.nullif(F.count(F.when(F.col("product_category") == "Sports", 1)), F.lit(0)), 2).alias("rn_plus_quantity_per_sports"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.grouping("rn_plus_quantity_tier").alias("gid"),
    )
    .filter(F.col("rn_plus_quantity_per_initech") > 0)
)
