from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "New York"]) | ~F.col("city").isin(["Berlin", "Singapore", "London"]))
    .select("country")
)
