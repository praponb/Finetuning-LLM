from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .pivot("channel", ["partner", "web", "mobile"])
    .agg(F.max("amount"))
)
