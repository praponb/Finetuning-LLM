from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 2.4) & (F.col("rating") >= 3.9) & F.col("rating").between(2.9, 4.1))
)
