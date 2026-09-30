from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").asc_nulls_first())
w2 = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")
w3 = Window.partitionBy("segment", "city").orderBy(F.col("age_rounded").asc_nulls_last(), "id")

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-05-07").cast("date")) & F.col("email").isNotNull())
    .withColumn("age_rounded", F.round("age", 1))
    .withColumn("country_name_key", F.concat_ws(",", "country", "name"))
)

with_windows = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w).alias("age_dense_rank"),
        F.ntile(4).over(w2).alias("age_bucket"),
    )
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy("country")
    .pivot("segment", ["Corporate", "SMB"])
    .agg(F.sum("age"))
)
