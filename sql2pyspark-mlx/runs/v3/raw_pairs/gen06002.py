from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .select("id", "launch_date", "cost", F.lead("cost", 1).over(w).alias("next_cost"))
)
