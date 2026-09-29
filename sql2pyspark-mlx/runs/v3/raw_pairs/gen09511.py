from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")

result = (
    df_departments
    .filter(F.col("budget") <= 3370000)
    .select(
        "id",
        "location",
        "budget",
        F.round((F.col("budget") / F.sum("budget").over(w)), 3).alias("budget_share"),
    )
)
