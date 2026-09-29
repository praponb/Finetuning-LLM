from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.lower("full_name").like("%lic%"))
    .select("id", "manager_id")
    .unionByName(df_employees.filter(F.length("email") > 10).select("id", "manager_id"))
    .distinct()
)
