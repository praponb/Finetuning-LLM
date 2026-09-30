from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2025-02-01", "2025-07-01"))
    .select("id", "region", "store_name")
    .unionByName(
        df_stores
        .filter(F.col("opened_date") < "2024-09-08")
        .select("id", "region", "store_name")
    )
)
