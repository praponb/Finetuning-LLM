from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date")
    .subtract(df_store_sales.select("sale_date"))
    .orderBy("hire_date")
)
