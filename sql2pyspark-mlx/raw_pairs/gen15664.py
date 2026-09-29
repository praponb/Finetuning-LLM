from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

cleaned = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.round((F.col("age") / F.col("credit_limit")), 2).alias("age_per_credit_limit"),
        F.when(F.col("country").isin(["DE", "SG"]), "Priority").otherwise("Other").alias("country_group"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.device"), F.col("c.country").alias("session_country"))
)
