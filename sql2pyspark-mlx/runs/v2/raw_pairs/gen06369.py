from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        (F.col("budget") - F.lag("budget", 1).over(w)).alias("budget_change"),
    )
)
