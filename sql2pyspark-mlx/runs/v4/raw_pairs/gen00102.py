from pyspark.sql import functions as F

result = (
    df_employees
    .select("manager_id")
    .exceptAll(
        df_employees
        .filter(F.length("full_name") > 6)
        .select("manager_id")
    )
)
