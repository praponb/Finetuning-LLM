from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_suppliers
    .filter(F.col("rating").isNotNull())
    .withColumn("supplier_name_city", F.concat("supplier_name", F.lit(" - "), "city"))
)

result = (
    valid
    .select(
        "*",
        F.last("rating", ignorenulls=True).over(w_running).alias("last_known_rating"),
        F.first("rating", ignorenulls=True).over(w2).alias("first_known_rating"),
        F.sum("rating").over(w_running).alias("running_rating"),
    )
)
