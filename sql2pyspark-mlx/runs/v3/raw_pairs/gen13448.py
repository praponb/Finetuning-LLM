from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") != "DE") & (F.col("city").isin(["Tokyo", "Berlin", "Chiang Mai"]) | F.col("supplier_name").like("%EuroGoods%")))
    .select("rating", "country")
    .orderBy("country")
)
