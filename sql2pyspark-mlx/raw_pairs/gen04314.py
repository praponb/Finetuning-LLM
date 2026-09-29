from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").asc_nulls_first(), "id")

deduped = (
    df_stores
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "store_name", "opened_date", "sqft")
)

result = (
    deduped
    .groupBy(
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
    )
    .agg(
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids") > 1)
)
