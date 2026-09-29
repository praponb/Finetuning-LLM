from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "London"]))
    .select("country")
    .distinct()
)
