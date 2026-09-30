from pyspark.sql import functions as F

max_shipped_date = df_shipments.filter(F.col("delivered_date").between("2023-03-14", "2025-10-01")).agg(F.max("shipped_date")).first()[0]

result = (
    df_shipments
    .filter(F.col("shipped_date") > max_shipped_date)
    .select("id", "shipped_date")
)
