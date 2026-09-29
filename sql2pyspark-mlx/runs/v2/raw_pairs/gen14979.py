from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-12-01", "2025-09-01"))
    .select("opened_date", "region")
    .unionByName(
        df_stores
        .filter(F.year("opened_date") >= 2024)
        .select("opened_date", "region")
    )
    .distinct()
    .orderBy(F.col("opened_date").desc())
)
