from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Tok%") & (F.col("city") == "Berlin") & F.col("rating").between(3.4, 4.1))
)
