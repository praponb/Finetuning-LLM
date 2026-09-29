from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_sessions
    .filter(F.to_date("started_at") >= "2024-06-25")
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.row_number().over(w).alias("rn"),
        F.last("country").over(w2).alias("last_country"),
    )
)
