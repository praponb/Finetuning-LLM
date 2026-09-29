from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue_per_unit").desc(), "id")

base = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-12-19")))
    .withColumn("revenue_per_unit", (F.col("revenue") + F.col("units")))
    .withColumn("units_rounded", F.round("units", 1))
)

ranked = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

ranked_store_sales = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

enriched = (
    ranked_store_sales
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.stock_qty"))
)

summary = (
    enriched
    .groupBy(
        F.when(F.col("stock_qty") >= 314, "PREMIUM").otherwise("STANDARD").alias("stock_qty_tier"),
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
        "store_id",
    )
    .agg(
        F.percentile_approx("revenue", 0.5).alias("revenue_p50"),
        F.bit_and("units").alias("units_bit_and"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("units_rounded").alias("total_units_rounded"),
    )
)

result = (
    summary
    .select(
        "stock_qty_tier",
        "quarter_start",
        "store_id",
        "units_bit_and",
        "units_quartiles",
        "total_units_rounded",
    )
    .orderBy(F.col("store_id").desc())
)
