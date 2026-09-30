from pyspark.sql import functions as F

result = (
    df_employees
    .select("manager_id")
    .intersect(
        df_employees
        .filter(F.upper(F.trim("email")) == "IVY@GMAIL.COM")
        .select("manager_id")
    )
)
