from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 4.1)
    .select("city")
    .subtract(df_stores.select("city"))
    .orderBy("city")
)
