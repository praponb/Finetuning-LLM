from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("supplier_name")
    .subtract(df_products.select("category"))
)
