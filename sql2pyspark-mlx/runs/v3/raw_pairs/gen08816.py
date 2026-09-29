from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([8, 9, 11]) & (F.col("carrier") == "UPS"))
    .orderBy(F.col("delivered_date").desc(), F.col("cost").desc(), "id")
    .limit(5)
)
