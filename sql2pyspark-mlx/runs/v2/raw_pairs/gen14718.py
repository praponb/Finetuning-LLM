from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city")
    .intersect(df_stores.filter(~F.col("store_name").like("Sia%")).select("city"))
    .orderBy("city")
)
