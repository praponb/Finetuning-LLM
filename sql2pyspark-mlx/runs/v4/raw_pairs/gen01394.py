from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.email").like("%@gmail.com") & ~F.col("c.country").isin(["UK", "TH"]))
    .select(
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("c.credit_limit"),
        F.col("c.email"),
        F.col("c.age"),
    )
    .orderBy("customer_id")
)
