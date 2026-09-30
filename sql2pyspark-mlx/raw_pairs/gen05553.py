from pyspark.sql import functions as F

matched_keys = (
    df_store_sales
    .select("store_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("store_id") == 4)
        .select("store_id", "id")
    )
)

reconciled = (
    df_store_sales
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("YEAR", "sale_date").alias("year_start"))
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.stddev_samp("units").alias("units_stddev"),
        F.bit_and("units").alias("units_bit_and"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
    )
    .orderBy(F.col("avg_units").asc_nulls_last())
)
