from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy("credit_limit", "id")

base = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-03-06").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.round((F.col("credit_limit") * 1.1), 2).alias("credit_limit_adjusted"),
        F.upper("segment").alias("segment_upper"),
    )
)

result = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
