from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([4, 5, 7]))
    .select("id", "customer_id", "plan")
    .unionByName(
        df_subscriptions
        .filter(F.month("end_date").isin([5, 9, 11]))
        .select("id", "customer_id", "plan")
    )
    .orderBy(F.col("id").desc())
)
