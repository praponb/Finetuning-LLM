from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([6, 9, 10]))
    .select("status")
    .unionByName(df_subscriptions.filter(F.col("plan") == "premium").select("status"))
    .distinct()
    .orderBy("status")
)
