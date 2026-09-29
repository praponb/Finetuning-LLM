from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 3.5)
    .select("city")
    .unionByName(df_stores.select("city"))
    .orderBy("city")
)
