from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select("start_date")
    .union(
        df_products
        .filter(~F.col("category").isin(["Grocery", "Electronics"]))
        .select("launch_date")
    )
    .orderBy("start_date")
)
