from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Li%"))
    .orderBy("country", "id")
    .limit(20)
)
