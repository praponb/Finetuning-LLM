from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("opened_date"), "city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_stores
    .filter(F.col("store_name").like("Dow%"))
    .select(
        "id",
        "opened_date",
        "city",
        "store_name",
        F.last("store_name").over(w).alias("last_store_name"),
    )
)
