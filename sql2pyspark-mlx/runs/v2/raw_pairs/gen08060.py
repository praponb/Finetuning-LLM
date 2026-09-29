from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() & F.col("start_date").between("2023-08-02", "2024-08-05"))
    .select("plan", "start_date")
    .orderBy("plan")
)
