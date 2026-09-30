from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "plan", F.explode("discounts").alias("tier_value"))

result = exploded.select("id", "plan")
