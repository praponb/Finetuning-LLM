from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2024-08-01")
    .select("status")
    .unionByName(
        df_shipments
        .filter(F.month("shipped_date").isin([3, 4, 9]))
        .select("status")
    )
    .distinct()
    .orderBy(F.col("status").desc())
)
