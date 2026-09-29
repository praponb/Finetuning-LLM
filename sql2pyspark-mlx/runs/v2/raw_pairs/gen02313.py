from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .select("id", "manager_id", "salary", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .select("id", "manager_id", "salary")
)
