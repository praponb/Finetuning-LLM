from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") != "DE") & ~F.col("country").isin(["UK", "JP"]))
)
