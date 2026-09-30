from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.1, 2.3))
    .select("city")
    .unionByName(df_stores.select("city"))
    .distinct()
    .orderBy("city")
)
