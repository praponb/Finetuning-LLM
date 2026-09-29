from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .unionByName(
        df_stores
        .filter(F.col("opened_date").between("2023-11-01", "2025-02-09"))
        .select("city")
    )
    .orderBy("city")
)
