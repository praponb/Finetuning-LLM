from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue_per_unit").desc_nulls_last(), "id")
w3 = Window.partitionBy("product_id").orderBy("rn")

valid = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4, 5]))
    .withColumn("units_tier", F.when(F.col("units") >= 37, "High").when(F.col("units") >= 7, "Medium").otherwise("Low"))
    .withColumn("revenue_per_unit", F.round((F.col("revenue") / F.col("units")), 2))
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

deduped_store_sales = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

ranked = (
    deduped_store_sales
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .alias("s")
    .join(df_stores.alias("dim"), F.col("s.store_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.city").alias("city"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("s.units")), 2).alias("total_units"),
    )
    .orderBy(F.col("rows_total").desc_nulls_last())
)
