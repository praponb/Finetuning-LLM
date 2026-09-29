from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter(F.col("segment").isNotNull() & (F.col("signup_date") <= "2023-03-26"))
    .select("*", F.last_day("signup_date").alias("signup_date_month_end"))
)

result = (
    filtered
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.country",
        "s.city",
        "s.credit_limit",
        "s.age",
        "s.signup_date_month_end",
        F.col("c.country").alias("session_country"),
    )
)
