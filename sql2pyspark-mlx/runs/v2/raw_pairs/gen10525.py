from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-01-01", "2024-06-10"))
    .select("delivered_date", "status")
    .unionByName(
        df_shipments
        .filter(F.year("shipped_date") >= 2024)
        .select("delivered_date", "status")
    )
    .distinct()
)
