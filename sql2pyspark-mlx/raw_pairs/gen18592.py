from pyspark.sql import functions as F

result = (
    df_departments
    .select("dept_name")
    .subtract(df_suppliers.filter(F.col("country") == "TH").select("supplier_name"))
    .orderBy("dept_name")
)
