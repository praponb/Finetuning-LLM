from pyspark.sql import functions as F

min_shipped_date = df_shipments.filter(F.col("cost") > 76).agg(F.min("shipped_date")).first()[0]

result = (
    df_shipments
    .filter(F.col("shipped_date") == min_shipped_date)
    .select("id", "shipped_date", "carrier")
    .orderBy(F.col("id").desc())
)
