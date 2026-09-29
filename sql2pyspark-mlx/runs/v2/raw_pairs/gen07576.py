from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.country"),
        F.col("s.browser"),
        F.col("c.signup_date"),
        F.when(F.col("s.browser").isin(["Chrome", "Edge"]), "Group A").otherwise("Other").alias("browser_group"),
    )
    .orderBy("browser_group")
)
