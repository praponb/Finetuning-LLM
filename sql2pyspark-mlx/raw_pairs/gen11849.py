from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").asc_nulls_first(), "id")

latest = (
    df_customers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "rank_in_group",
    )
    .orderBy(F.col("rank_in_group").desc())
)
