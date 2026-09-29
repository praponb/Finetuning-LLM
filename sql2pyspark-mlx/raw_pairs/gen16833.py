from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").asc_nulls_first(), "id")

valid = (
    df_customers
    .filter(F.col("country") != "TH")
    .select(
        "id",
        "name",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.round((F.col("age") / F.col("credit_limit")), 2).alias("age_per_credit_limit"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

result = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
