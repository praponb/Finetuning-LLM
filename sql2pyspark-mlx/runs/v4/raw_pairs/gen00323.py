from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "plan", F.explode("discounts").alias("tier_value"))

result = lines.select("id")
