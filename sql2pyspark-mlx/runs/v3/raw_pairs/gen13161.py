from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .select("end_date")
    .subtract(df_shipments.select("shipped_date"))
    .orderBy("end_date")
)
