from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") != "Berlin")
    .select("opened_date")
    .unionByName(
        df_stores
        .filter(F.col("opened_date").between("2025-05-08", "2025-08-01"))
        .select("opened_date")
    )
    .distinct()
)
