from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.segment") == "SMB") & F.col("c.country").isin(["UK", "US"]))
    .select(
        F.col("s.monthly_fee"),
        F.col("s.start_date"),
        F.col("c.age"),
        F.col("c.country"),
        F.col("c.name"),
        F.regexp_extract(F.col("c.email"), "@(.+)$", 1).alias("email_domain"),
    )
)
