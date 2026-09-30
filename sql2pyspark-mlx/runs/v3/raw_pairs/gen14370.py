from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date")
    .union(df_stores.select("opened_date"))
    .orderBy(F.col("delivered_date").desc())
)
