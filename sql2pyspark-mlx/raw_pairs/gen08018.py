from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.0, 3.0))
    .select("supplier_name")
    .subtract(df_customers.select("name"))
)
