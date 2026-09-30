from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.month("shipped_date").isin([2, 3, 5]) & (F.col("cost") <= 58)) | ~F.col("carrier").isin(["UPS", "Kerry"]))
    .orderBy("shipped_date")
)
