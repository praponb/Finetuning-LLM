from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "DHL") & (F.month("delivered_date").isin([1, 3, 11]) | F.col("cost").between(64, 91)))
    .select("id", "delivered_date", "cost")
)
