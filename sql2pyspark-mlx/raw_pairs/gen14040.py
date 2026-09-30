from pyspark.sql import functions as F

result = (
    df_employees
    .select("level", "id")
    .intersectAll(
        df_employees
        .filter(~F.col("is_remote"))
        .select("level", "id")
    )
)
