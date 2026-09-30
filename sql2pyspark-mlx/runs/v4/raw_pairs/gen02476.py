from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city").orderBy("signup_date")

filtered = (
    df_customers
    .filter(F.col("email").isNotNull())
    .select(
        "*",
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

result = (
    filtered
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)
