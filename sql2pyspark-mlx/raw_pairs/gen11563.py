from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.opened_date").between("2024-06-25", "2024-08-18") & F.col("a.balance").between(44000, 55000))
    .select(
        F.col("c.credit_limit"),
        F.col("c.city"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
    )
    .orderBy(F.col("city").desc())
)
