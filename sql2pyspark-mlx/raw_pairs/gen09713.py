from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([1, 4, 7]))
    .select("delivered_date")
    .intersect(df_subscriptions.filter(F.col("plan") == "basic").select("end_date"))
)
