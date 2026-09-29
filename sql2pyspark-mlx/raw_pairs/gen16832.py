from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.month(F.col("c.signup_date")).isin([4, 8, 9]))
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("c.age"),
        F.col("c.email"),
        F.col("c.name"),
        F.upper(F.col("c.segment")).alias("segment_upper"),
    )
    .orderBy("segment_upper", "email")
)
