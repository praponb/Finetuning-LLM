from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("units").desc())

deduped = (
    df_store_sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

kept_keys = (
    ranked
    .select("store_id")
    .subtract(
        df_store_sales
        .filter(F.year("sale_date") == 2025)
        .select("store_id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("WEEK", "sale_date").alias("week_start"), "product_id")
    .agg(
        F.bit_or("units").alias("units_bit_or"),
        F.count("*").alias("cnt"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
    )
    .orderBy("corr_revenue_units")
)
