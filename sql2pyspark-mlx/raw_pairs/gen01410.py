from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 4.1)
    .select("city")
    .unionByName(
        df_stores
        .filter(F.col("city").isin(["Tokyo", "Chiang Mai", "Singapore"]))
        .select("city")
    )
)
