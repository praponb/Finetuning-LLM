from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .filter(F.col("browser") == "Edge")
    .select(
        "id",
        "started_at",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
    )
)
